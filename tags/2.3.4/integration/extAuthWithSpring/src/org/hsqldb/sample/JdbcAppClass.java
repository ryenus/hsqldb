/* Copyright (c) 2001-2010, The HSQL Development Group
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


package org.hsqldb.sample;

import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Connection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import javax.sql.DataSource;

/**
 * An application class that performs some simple JDBC work.
 *
 * This class is purposefully not Spring-aware.
 */
public class JdbcAppClass {
    private static Log log = LogFactory.getLog(JdbcAppClass.class);

    private boolean initialized;
    private DataSource ds;

    public void init() {
        if (ds == null) throw new IllegalStateException(
                "Required property 'dataSource' not set");
        initialized = true;
    }

    public void setDataSource(DataSource ds) {
        this.ds = ds;
    }

    public void doJdbcWork() throws SQLException {
        if (!initialized)
            throw new IllegalStateException(JdbcAppClass.class.getName()
                    + " instance not initialized");
        Connection c = null;
        ResultSet rs = null;
        try {
            c = ds.getConnection();
            rs = c.createStatement().executeQuery("SELECT * FROM t1");
            if (!rs.next()) {
                log.error("App class failed to retrieve data from catalog");
                return;
            }
            if (rs.getInt(1) != 456) {
                log.error("App class retrieved wrong value: "  + rs.getInt(1));
                return;
            }
            if (rs.next()) {
                log.error("App class failed too much data from catalog");
                return;
            }
        } finally {
            if (c != null) try {
                c.rollback();
            } catch (SQLException se) {
                // Intentionally empty.
                // We have done nothing that we want to commit, but want to
                // aggressively free transactional resources.
            }
            if (rs != null) try {
                rs.close();
            } catch (SQLException se) {
                log.error("Failed to close emulation database setup Connection",
                        se);
            } finally {
                rs = null;  // Encourage GC
            }
            if (c != null) try {
                c.close();
            } catch (SQLException se) {
                log.error("Failed to close emulation database setup Connection",
                        se);
            } finally {
                c = null;  // Encourage GC
            }
        }
        log.info("Application Success");
    }
}
