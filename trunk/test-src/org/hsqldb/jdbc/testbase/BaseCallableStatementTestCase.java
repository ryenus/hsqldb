// <editor-fold defaultstate="collapsed" desc="Copyright Notice & Disclaimer">
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
// </editor-fold>

package org.hsqldb.jdbc.testbase;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.text.MessageFormat;

/**
 * Provides...
 *
 * @author Campbell Boucher-Burnet <campbell.boucherburnet@gov.sk.ca>;
 * @version 1.0
 * @since 1.0
 */
public class BaseCallableStatementTestCase extends BaseJdbcTestCase {
    public BaseCallableStatementTestCase(String testName) {
        super(testName);
    }

    protected String getSetObjectTestCall(String typeName) {
        return MessageFormat.format("select cast(? as {0}) from dual", new Object[]{ typeName});
    }

    protected boolean isTestOutParameters() {
        return super.getBooleanProperty("test.callable.statement.out.parameters", true);
    }

    protected CallableStatement prepRegAndExec(String call, int index, int type) throws Exception {
        CallableStatement stmt = prepareCall(call);
        stmt.registerOutParameter(index, type);
        stmt.execute();
        return stmt;
    }

    protected CallableStatement prepareCall(String call) throws Exception {
        return connectionFactory().prepareCall(call, newConnection());
    }

    protected void setObjectTest(String typeName, Object x, int type) throws Exception {
        CallableStatement stmt = prepareCall(getSetObjectTestCall(typeName));
        stmt.setObject(1, x, type);
        ResultSet rs = stmt.executeQuery();
        rs.next();
        Object result = rs.getObject(1);
        if (x instanceof Number) {
            assertEquals(((Number) x).doubleValue(), ((Number) result).doubleValue());
        } else if (x != null && x.getClass().isArray()) {
            assertJavaArrayEquals(x, result);
        } else {
            assertEquals(x, result);
        }
    }
}
