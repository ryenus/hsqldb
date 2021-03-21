/* Copyright (c) 2001-2020, The HSQL Development Group
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


package org.hsqldb.test;

/**
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 */
interface TestConnectionSettings {

    String url();

    String dbPath();

    String connType();

    boolean isServlet();

    class TestConnectionSettingsMem implements TestConnectionSettings {

        public String url() {
            return "jdbc:hsqldb:mem:test;sql.enforce_strict_size=true;sql.restrict_exec=true;hsqldb.tx=mvcc";
        }

        public String dbPath() {
            return "mem:test;sql.enforce_strict_size=true;sql.restrict_exec=true;hsqldb.tx=mvcc";
        }

        public String connType() {
            return "mem:";
        }

        public boolean isServlet() {
            return false;
        }
    }

    class TestConnectionSettingsFile implements TestConnectionSettings {

        public String url() {
            return TestDirectorySettings.fileBaseURL
                   + "unitestdb;sql.enforce_strict_size=true;sql.restrict_exec=true;hsqldb.tx=mvcc";
        }

        public String dbPath() {
            return TestDirectorySettings.fileBase + "unitestdb";
        }

        public String connType() {
            return "file:";
        }

        public boolean isServlet() {
            return false;
        }
    }

    class TestConnectionSettingsServerMem implements TestConnectionSettings {

        public String url() {
            return "jdbc:hsqldb:hsql://localhost/test";
        }

        public String dbPath() {
            return "mem:test;sql.enforce_strict_size=true;sql.restrict_exec=true;hsqldb.tx=mvcc";
        }

        public String connType() {
            return "hsql:";
        }

        public boolean isServlet() {
            return false;
        }
    }

    class TestConnectionSettingsHttpServerMem
    implements TestConnectionSettings {

        public String url() {
            return "jdbc:hsqldb:http://localhost:8085/test";
        }

        public String dbPath() {
            return "mem:test;sql.enforce_strict_size=true;sql.restrict_exec=true;hsqldb.tx=mvcc";
        }

        public String connType() {
            return "http:";
        }

        public boolean isServlet() {
            return false;
        }
    }

    class TestConnectionSettingsServerFile implements TestConnectionSettings {

        public String url() {
            return "jdbc:hsqldb:hsql://localhost/test";
        }

        public String dbPath() {
            return TestDirectorySettings.fileBase
                   + "unitestdb;sql.enforce_strict_size=true;sql.restrict_exec=true;hsqldb.tx=mvcc";
        }

        public String connType() {
            return "hsql:";
        }

        public boolean isServlet() {
            return false;
        }
    }

    class TestConnectionSettingsHttpServletMem
    implements TestConnectionSettings {

        public String url() {
            return "jdbc:hsqldb:http://localhost:8080/test";
        }

        public String dbPath() {
            return "mem:test;sql.enforce_strict_size=true;sql.restrict_exec=true;hsqldb.tx=mvcc";
        }

        public String connType() {
            return "http:";
        }

        public boolean isServlet() {
            return true;
        }
    }
}
