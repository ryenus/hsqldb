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


package org.hsqldb.auth;

import org.hsqldb.lib.FrameworkLogger;

/**
 * Delegates authentication decisions, and optionally determination of user
 * roles and schema, to a different HyperSQL catalog, which may be in the same
 * JVM or remote.
 *
 * @see AuthFunctionBean
 * @see #initialize()
 */
public class HsqldbSlaveAuthBean implements AuthFunctionBean {
    private static FrameworkLogger logger =
            FrameworkLogger.getLog(HsqldbSlaveAuthBean.class);

    private String masterJdbcUrl;
    private boolean delegateRolesSchema = true;
    protected boolean initialized;

    public void setMasterJdbcUrl(String masterJdbcUrl) {
        this.masterJdbcUrl = masterJdbcUrl;
    }

    /**
     * Defaults to true
     */
    public void setDelegateRolesSchema(boolean doDelegateRolesSchema) {
        delegateRolesSchema = doDelegateRolesSchema;
    }

    public HsqldbSlaveAuthBean() {
        // Intentionally empty
    }

    /**
     * @throws IllegalStateException if any required setting has not been set.
     */
    public void init() {
        if (masterJdbcUrl == null) {
            throw new IllegalStateException(
                    "Required property 'masterJdbcUrl' not set");
        }
        initialized = true;
    }

    /**
     * @see AuthFunctionBean#authenticate(String, password)
     */
    public String[] authenticate(String userName, String password)
            throws DenyException {
        if (!initialized) {
            throw new IllegalStateException(
                "You must invoke the 'init' method to initialize the "
                + HsqldbSlaveAuthBean.class.getName() + " instance.");
        }
        logger.severe("CLASS NOT IMPLEMENTED YET!");
        throw new DenyException();
    }
}
