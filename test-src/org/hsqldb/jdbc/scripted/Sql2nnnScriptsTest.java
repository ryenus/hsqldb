/* Copyright (c) 2001-2021, The HSQL Development Group
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
package org.hsqldb.jdbc.scripted;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.testbase.BaseScriptedTestCase;
import org.hsqldb.testbase.HsqldbEmbeddedDatabaseDeleter;
import org.hsqldb.testbase.ResourceCollector;
import org.hsqldb.testbase.StreamUtil;

/**
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
public class Sql2nnnScriptsTest extends BaseScriptedTestCase {

    private static final Sql2nnnScriptsTest instance = new Sql2nnnScriptsTest();

    private static final String URL = "jdbc:hsqldb:file:sql2nnn-test/";

    private static final Logger LOG = Logger.getLogger(Sql2nnnScriptsTest.class.getName());

    private Sql2nnnScriptsTest() {
        super();
    }

    public Sql2nnnScriptsTest(String script) {
        super(script);

    }

    @Override
    public String getUrl() {
        return URL;
    }

    @Override
    protected void preTearDown() throws Exception {
        super.preTearDown();
    }

    @Override
    protected void postTearDown() throws Exception {
        super.postTearDown();
    }

    public static Test suite() {
        final boolean success = HsqldbEmbeddedDatabaseDeleter.deleteDatabase(URL);

        if (success) {
            LOG.log(Level.INFO, "Database deletion succeeded for: {0}", URL);
        } else {
            LOG.log(Level.SEVERE, "Database deletion failed for: {0}", URL);
        }

        return instance.getSuite();
    }

    protected Test getSuite() {
        final TestSuite suite = new TestSuite(Sql2nnnScriptsTest.class.getSimpleName());
        try {

            URL location = StreamUtil.streamOf(getClass().getClassLoader().getResources("sql2nnn/")).findFirst().orElse(null);

            Pattern nameMatcher = Pattern.compile("^.*\\.sql", Pattern.CASE_INSENSITIVE);

            ResourceCollector rc = new ResourceCollector(nameMatcher);
            List<String> resources = new ArrayList<>();
            try {
                rc.collectResources(location, resources);
            } catch (IOException | URISyntaxException ex) {
                Logger.getLogger(Sql2nnnScriptsTest.class.getName()).log(Level.SEVERE, null, ex);
            }

            //resources.forEach(resource -> suite.addTest(new TestSelfScriptsTest(resource)));

            return suite;
        } catch (IOException ex) {
            Logger.getLogger(Sql2nnnScriptsTest.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return suite;
    }

    public static void main(String[] args) throws Exception {
        junit.textui.TestRunner.run(suite());
    }
}
