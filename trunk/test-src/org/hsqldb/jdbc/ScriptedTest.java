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
package org.hsqldb.jdbc;

import java.io.IOException;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.testbase.BaseScriptedTestCase;
import org.hsqldb.testbase.HsqldbEmbeddedDatabaseDeleter;

/**
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
public class ScriptedTest extends BaseScriptedTestCase {

    private static final ScriptedTest instance = new ScriptedTest();

    private HsqldbEmbeddedDatabaseDeleter m_deleter = new HsqldbEmbeddedDatabaseDeleter(getUrl());

    private ScriptedTest() {
        super();
    }

    public ScriptedTest(String script) {
        super(script);
    }

    @Override
    public String getUrl() {
        return "jdbc:hsqldb:file:scripted-test";
    }

    @Override
    protected void preTearDown() throws Exception {
        super.preTearDown();
        this.connectionFactory().addEventListener(m_deleter);
    }

    @Override
    protected void postTearDown() throws Exception {
        super.postTearDown();
        this.connectionFactory().removeEventListener(m_deleter);
    }

    /**
     *
     * @return
     */
    public static Test suite() {
        return instance.getSuite();
    }

    /**
     *
     * @return
     */
    @SuppressWarnings("CallToThreadDumpStack")
    protected Test getSuite() {
        TestSuite suite = new TestSuite("ScriptTest");

        try {

            String[] resources = getResoucesInPackage(
                    "org.hsqldb.jdbc.resources.sql");

            for (int i = 0; i < resources.length; i++) {
                suite.addTest(new ScriptedTest(resources[i]));
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return suite;
    }

    /**
     * @param args
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        junit.textui.TestRunner.run(suite());
    }
}
