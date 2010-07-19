package org.hsqldb.testbase;

import org.hsqldb.jdbc.JDBCConnection;

/**
 *
 * @author cboucher
 */
@ForSubject(JDBCConnection.class)
public class ATest extends BaseTestCase {

    @OfMethod("something()")
    public void testSomething() {
    }
}
