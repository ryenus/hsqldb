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

import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.testbase.BaseTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

/* $Id$ */

/**
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
@ForSubject(Option.class)
public class OptionTest extends BaseTestCase {

    public OptionTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(OptionTest.class);

        return suite;
    }

    /**
     * Test of isDefault method, of class org.hsqldb.util.preprocessor.Option.
     */
    @OfMethod("isDefault(int)")
    public void testIsDefault() {
        assertEquals(true,  Option.isDefault(Option.DEFAULT));

        int start = Option.BACKUP;
        int end   = Option.BACKUP |
                    Option.FILTER |
                    Option.INDENT |
                    Option.TEST_ONLY |
                    Option.VERBOSE;

        for (int options = start; options <= end; options++) {
            assertEquals(true, !Option.isDefault(options));
        }
    }

    /**
     * Test of setDefault method, of class org.hsqldb.util.preprocessor.Option.
     */
    @OfMethod("setDefault(int,boolean)")
    public void testSetDefault() {
        int     options   = ~Option.DEFAULT;
        boolean _default  = true;
        int     expResult = Option.DEFAULT;
        int     result    = Option.setDefault(options, _default);

        assertEquals(expResult, result);
    }

    /**
     * Test of isBackup method, of class org.hsqldb.util.preprocessor.Option.
     */
    @OfMethod("isBackup(int)")
    public void testIsBackup() {
        int     options   = Option.BACKUP;
        boolean expResult = true;
        boolean result    = Option.isBackup(options);

        assertEquals(expResult, result);
    }

    /**
     * Test of setBackup method, of class org.hsqldb.util.preprocessor.Option.
     */
    @OfMethod("setBackup(int,boolean)")
    public void testSetBackup() {
        int options    = Option.DEFAULT;
        boolean backup = true;
        int expResult  = Option.BACKUP;
        int result     = Option.setBackup(options, backup);

        assertEquals(expResult, result);
    }

    /**
     * Test of isFilter method, of class org.hsqldb.util.preprocessor.Option.
     */
     @OfMethod("isFilter(int)")
    public void testIsFilter() {
        int     options   = Option.FILTER;
        boolean expResult = true;
        boolean result    = Option.isFilter(options);

        assertEquals(expResult, result);
    }

    /**
     * Test of setFilter method, of class org.hsqldb.util.preprocessor.Option.
     */
    @OfMethod("setFilter(int,boolean)")
    public void testSetFilter() {
        int     options   = Option.DEFAULT;
        boolean filter    = true;
        int     expResult = Option.FILTER;
        int     result    = Option.setFilter(options, filter);

        assertEquals(expResult, result);
    }

    /**
     * Test of isIndent method, of class org.hsqldb.util.preprocessor.Option.
     */
     @OfMethod("isIdent(int)")
    public void testIsIndent() {
        int     options   = Option.INDENT;
        boolean expResult = true;
        boolean result    = Option.isIndent(options);

        assertEquals(expResult, result);
    }

    /**
     * Test of setIndent method, of class org.hsqldb.util.preprocessor.Option.
     */
    @OfMethod("setIdent(int,boolean)")
    public void testSetIndent() {
        int     options   = Option.DEFAULT;
        boolean indent    = true;
        int     expResult = Option.INDENT;
        int     result    = Option.setIndent(options, indent);

        assertEquals(expResult, result);
    }

    /**
     * Test of isTestOnly method, of class org.hsqldb.util.preprocessor.Option.
     */
     @OfMethod("isTestOnly(int)")
    public void testIsTestOnly() {
        int     options   = Option.TEST_ONLY;
        boolean expResult = true;
        boolean result    = Option.isTestOnly(options);

        assertEquals(expResult, result);
    }

    /**
     * Test of setTestOnly method, of class org.hsqldb.util.preprocessor.Option.
     */
    @OfMethod("setTestOnly(int,boolean)")
    public void testSetTestOnly() {
        int     options   = Option.DEFAULT;
        boolean testOnly  = true;
        int     expResult = Option.TEST_ONLY;
        int     result    = Option.setTestOnly(options, testOnly);

        assertEquals(expResult, result);
    }

    /**
     * Test of isVerbose method, of class org.hsqldb.util.preprocessor.Option.
     */
     @OfMethod("isVerbose(int)")
    public void testIsVerbose() {
        int     options   = Option.VERBOSE;
        boolean expResult = true;
        boolean result    = Option.isVerbose(options);

        assertEquals(expResult, result);
    }

    /**
     * Test of setVerbose method, of class org.hsqldb.util.preprocessor.Option.
     */
    @OfMethod("setVerbose(int,boolean)")
    public void testSetVerbose() {
        int     options   = Option.DEFAULT;
        boolean verbose   = true;
        int     expResult = Option.VERBOSE;
        int     result    = Option.setVerbose(options, verbose);

        assertEquals(expResult, result);
    }
}
