package org.hsqldb.jdbc.pool;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.jdbc.testbase.BaseJdbcTestCase;

/**
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 */
public class JDBCXIDTest extends BaseJdbcTestCase {

    public static final int FORMAT_ID_NULL = -1;
    public static final int FORMAT_ID_OSI_CCR = 0;
    public static final int FORMAT_ID_JONAS_1 = 0xBB14;
    public static final int FORMAT_ID_JONAS_2 = 0xBB20;
    public static final int FORMAT_ID_JBOSS = 0x0101;

    public JDBCXIDTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(JDBCXIDTest.class);
        return suite;
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    protected int getFormatId() {

        return 0;
    }

    /**
     * Test of getFormatId method, of class JDBCXID.
     */
    public void testGetFormatId() {
        stubTestResult();
    }

    /**
     * Test of getGlobalTransactionId method, of class JDBCXID.
     */
    public void testGetGlobalTransactionId() {
        stubTestResult();
    }

    /**
     * Test of getBranchQualifier method, of class JDBCXID.
     */
    public void testGetBranchQualifier() {
        stubTestResult();
    }

    /**
     * Test of hashCode method, of class JDBCXID.
     */
    public void testHashCode() {
        stubTestResult();
    }

    /**
     * Test of equals method, of class JDBCXID.
     */
    public void testEquals() {
       stubTestResult();
    }
}
