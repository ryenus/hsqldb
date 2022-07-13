/* Copyright (c) 2001-2022, The HSQL Development Group
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


package org.hsqldb;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.LongKeyHashMap;
import org.hsqldb.lib.ObjectComparator;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultMetaData;

/**
 * This class manages the reuse of Statement objects for prepared
 * statements for a Session instance.<p>
 *
 * A compiled statement is registered by a session to be managed.<p>
 *
 * The sql statement text distinguishes different compiled statements and acts
 * as lookup key when a session initially looks for an existing instance of
 * the compiled sql statement.<p>
 *
 * The unique compiled statement id for the sql statement is used to access the
 * statement.<p>
 *
 * Changes to database structure via DDL statements, will result in all
 * registered Statement objects to become invalidated. This is done by
 * comparing the schema change and compile timestamps. When a session
 * subsequently attempts to use an invalidated Statement via its id, it will
 * reinstantiate the Statement using its sql statement still held by this class.<p>
 *
 * This class keeps count of the number of time each registered compiled
 * statement is linked to a session. It unregisters a compiled statement when
 * it is not in use.<p>
 *
 * Modified by fredt@users from the original by campbell-burnet@users to
 * simplify, support multiple identical prepared statements per session, and
 * avoid memory leaks. Changed implementation to a session object for optimised
 * access.<p>
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 *
 * @version 2.7.0
 * @since 1.7.2
 */
public final class StatementManager {

    /** The Database for which this is managing Statement objects. */
    private Database database;

    /** The Session for which this is managing Statement objects */
    private Session session;

    /** Set of wrappers for Statement object. */
    private HashSet<StatementWrapper> statementSet;

    /** Map: Statement id (int) maps to: wrapper for Statement object. */
    private LongKeyHashMap csidMap;

    /**
     * Monotonically increasing counter used to assign unique ids to
     * Statement objects.
     */
    private long next_cs_id;

    /**
     * Constructs a new instance of <code>StatementManager</code>.
     *
     * @param session the session instance for which this object is to
     *      manage Statement objects.
     */
    StatementManager(Session session) {

        this.session  = session;
        this.database = session.database;
        statementSet  = new HashSet(32, new StatementComparator());
        csidMap       = new LongKeyHashMap();
        next_cs_id    = 0;
    }

    /**
     * Clears all internal data structures, removing any references to Statements.
     */
    void reset() {

        statementSet.clear();
        csidMap.clear();

        next_cs_id = 0;
    }

    /**
     * Retrieves the next Statement identifier in the sequence.
     *
     * @return the next Statement identifier in the sequence.
     */
    private long nextID() {

        next_cs_id++;

        return next_cs_id;
    }

    /**
     * Returns an existing Statement object with the given
     * statement identifier. Returns null if the Statement object
     * has expired and cannot be recompiled
     *
     * @param csid the identifier of the requested Statement object
     * @return the requested Statement object
     */
    public Statement getStatement(long csid) {

        StatementWrapper sw = (StatementWrapper) csidMap.get(csid);

        if (sw == null) {
            return null;
        }

        return getStatement(sw);
    }

    private Statement getStatement(StatementWrapper sw) {

        Statement statement = sw.statement;

        if (statement.getCompileTimestamp()
                < database.schemaManager.getSchemaChangeTimestamp()) {
            Statement newStatement = recompileStatement(statement);

            if (newStatement == null) {
                removeStatement(statement.getID());

                return null;
            }

            newStatement.setCompileTimestamp(
                database.txManager.getSystemChangeNumber());

            sw.statement = newStatement;

            return newStatement;
        }

        return sw.statement;
    }

    /**
     * Recompiles an existing statement.
     *
     * Used by transaction manager for all statements, prepared or not prepred.
     *
     * @param statement the old expired statement
     * @return the requested Statement object
     */
    public Statement getStatement(Statement statement) {

        long csid = statement.getID();

        if (csid != 0) {
            StatementWrapper sw = (StatementWrapper) csidMap.get(csid);

            if (sw != null) {
                return getStatement(sw);
            }
        }

        return recompileStatement(statement);
    }

    /**
     * Returns an up-to-date Statement using the SQL and settings of
     * the original. Returns null if the SQL cannot be compiled.
     *
     * @param cs the old expired statement
     * @return the new Statement object
     */
    private Statement recompileStatement(Statement cs) {

        HsqlName  oldSchema = session.getCurrentSchemaHsqlName();
        Statement newStatement;

        // revalidate with the original schema
        try {
            HsqlName schema = cs.getSchemaName();
            int      props  = cs.getCursorPropertiesRequest();

            if (schema != null) {

                // checks the old schema exists
                session.setSchema(schema.name);
            }

            boolean setGenerated = cs.generatedResultMetaData() != null;

            newStatement = session.compileStatement(cs.getSQL(), props);

            newStatement.setCursorPropertiesRequest(props);

            if (!cs.getResultMetaData().areTypesCompatible(
                    newStatement.getResultMetaData())) {
                return null;
            }

            if (!cs.getParametersMetaData().areTypesCompatible(
                    newStatement.getParametersMetaData())) {
                return null;
            }

            newStatement.setCompileTimestamp(
                database.txManager.getSystemChangeNumber());

            if (setGenerated) {
                StatementDML si = (StatementDML) cs;

                newStatement.setGeneratedColumnInfo(si.generatedType,
                                                    si.generatedInputMetaData);
            }
        } catch (Throwable t) {
            return null;
        } finally {
            session.setCurrentSchemaHsqlName(oldSchema);
        }

        return newStatement;
    }

    /**
     * Registers a Statement to be managed.
     *
     * @param wrapper the wrapper for the Statement to add
     * @return The statement id assigned to the Statement object
     */
    private long registerStatement(StatementWrapper wrapper) {

        Statement cs = wrapper.statement;

        cs.setCompileTimestamp(database.txManager.getSystemChangeNumber());

        long csid = nextID();

        cs.setID(csid);
        statementSet.add(wrapper);
        csidMap.put(csid, wrapper);

        return csid;
    }

    /**
     * Removes a link between a PreparedStatement and a Statement. If the
     * statement is not linked with any other PreparedStatement, it is
     * removed from management.
     *
     * @param csid the Statement identifier
     */
    void freeStatement(long csid) {

        StatementWrapper sw = (StatementWrapper) csidMap.get(csid);

        if (sw == null) {
            return;
        }

        sw.usageCount--;

        if (sw.usageCount == 0) {
            removeStatement(csid);
        }
    }

    /**
     * Removes an invalidated Statement.
     *
     * @param csid the Statement identifier
     */
    private void removeStatement(long csid) {

        if (csid <= 0) {

            // statement was never added
            return;
        }

        StatementWrapper sw = (StatementWrapper) csidMap.remove(csid);

        if (sw != null) {
            statementSet.remove(sw);
        }
    }

    /**
     * Compiles an SQL statement and returns a Statement Object
     *
     * @param cmd the Result holding the SQL
     * @return Statement
     */
    Statement compile(Result cmd) {

        StatementWrapper newWrapper = new StatementWrapper();

        newWrapper.sql               = cmd.getMainString();
        newWrapper.cursorProps       = cmd.getExecuteProperties();
        newWrapper.generatedType     = cmd.getGeneratedResultType();
        newWrapper.generatedMetaData = cmd.getGeneratedResultMetaData();
        newWrapper.schemaName        = session.currentSchema;

        StatementWrapper wrapper = statementSet.get(newWrapper);

        if (wrapper != null) {
            if (wrapper.statement.getCompileTimestamp()
                    >= database.schemaManager.getSchemaChangeTimestamp()) {
                wrapper.usageCount++;

                return wrapper.statement;
            }

            // old version is invalid
            removeStatement(wrapper.statement.getID());
        }

        wrapper = newWrapper;
        wrapper.statement = session.compileStatement(wrapper.sql,
                wrapper.cursorProps);

        wrapper.statement.setCursorPropertiesRequest(wrapper.cursorProps);
        wrapper.statement.setGeneratedColumnInfo(cmd.getGeneratedResultType(),
                cmd.getGeneratedResultMetaData());
        registerStatement(wrapper);

        wrapper.usageCount = 1;

        return wrapper.statement;
    }

    private static class StatementComparator
        implements ObjectComparator<StatementWrapper> {

        public boolean equals(StatementWrapper s1, StatementWrapper s2) {

            return s1.sql.equals(s2.sql)
                   && s1.schemaName.equals(s2.schemaName)
                   && s1.cursorProps == s2.cursorProps
                   && s1.generatedType == s2.generatedType
                   && ResultMetaData.areGeneratedReguestsCompatible(
                       s1.generatedMetaData, s2.generatedMetaData);
        }

        public int hashCode(StatementWrapper a) {
            return a.sql.hashCode();
        }

        public long longKey(StatementWrapper a) {
            return 0L;
        }
    }

    private static class StatementWrapper {

        String         sql;
        HsqlName       schemaName;
        int            cursorProps;
        int            generatedType;
        ResultMetaData generatedMetaData;
        Statement      statement;
        long           usageCount;
    }
}
