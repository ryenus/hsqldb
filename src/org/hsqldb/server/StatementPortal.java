/* Copyright (c) 2001-2009, The HSQL Development Group
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


package org.hsqldb.server;

import java.util.Map;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultConstants;
import org.hsqldb.Session;

class StatementPortal {
    public Object[] parameters;
    public Result bindResult, ackResult;
    public String lcQuery;
    public String handle;
    private Map containingMap;
    private Session session;

    /**
     * Convenience wrapper for the 3-param constructor.
     *
     * @see #StatementPortal(String, OdbcPreparedStatement, String[], Map)
     */
    public StatementPortal(String handle,
    OdbcPreparedStatement odbcPs, Map containingMap)
    throws RecoverableOdbcFailure {
        this(handle, odbcPs, new String[0], containingMap);
    }

    /**
     * Instantiates a proxy ODBC StatementPortal object for the
     * Connection Session, and adds the new instance to the specified map.
     */
    public StatementPortal(String handle, OdbcPreparedStatement odbcPs,
    String[] paramStrings, Map containingMap) throws RecoverableOdbcFailure {
        this.handle = handle;
        lcQuery = odbcPs.query.toLowerCase();
        ackResult = odbcPs.ackResult;
        session = odbcPs.session;
        this.containingMap = containingMap;
        bindResult = Result.newPreparedExecuteRequest(
            odbcPs.ackResult.parameterMetaData.getParameterTypes(),
            odbcPs.ackResult.getStatementID());
        switch (bindResult.getType()) {
            case ResultConstants.EXECUTE:
                break;
            case ResultConstants.ERROR:
                throw new RecoverableOdbcFailure(bindResult);
            default:
                throw new RecoverableOdbcFailure(
                    "Output Result from seconary Statement prep is of "
                    + "unexpected type: " + bindResult.getType());
        }
        if (paramStrings.length < 1) {
            parameters = new Object[0];
        } else {
            org.hsqldb.result.ResultMetaData pmd =
                odbcPs.ackResult.parameterMetaData;
            if (pmd == null) {
                throw new RecoverableOdbcFailure("No metadata for Result ack");
            }
            org.hsqldb.types.Type[] paramTypes = pmd.getParameterTypes();
            if (paramTypes.length != paramStrings.length) {
                throw new RecoverableOdbcFailure(null,
                    "Client didn't specify all " + paramTypes.length
                    + " parameters (" + paramStrings.length + ')', "08P01");
            }
            parameters = new Object[paramStrings.length];
            try {
                for (int i = 0; i < parameters.length; i++) {
                    parameters[i] = new PgType(paramTypes[i])
                        .getParameter(paramStrings[i], session);
                }
            } catch (java.sql.SQLException se) {
                throw new RecoverableOdbcFailure("Typing failure: " + se);
            }
        }
        containingMap.put(handle, this);
    }

    /**
     * Releases resources for this instance
     * and removes this instance from the containing map.
     */
    public void close() {
        // TODO:  Free up resources!
        containingMap.remove(handle);
    }
}
