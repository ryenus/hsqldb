package org.hsqldb.server;

import java.util.Arrays;
import java.util.logging.Logger;
import org.hsqldb.testbase.BaseTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

/**
 *
 * @author campbell
 */
@ForSubject(ServerProperties.class)
public class ServerPropertiesTest extends BaseTestCase {

    private static final Logger LOG = Logger.getLogger(ServerPropertiesTest.class.getName());

    public ServerPropertiesTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Test of validate method, of class ServerProperties.
     */
    @OfMethod("validate()")
    public void testValidate() {
        ServerProperties p = new ServerProperties(ServerConstants.SC_PROTOCOL_HSQL);
        p.setProperty("invalid.key", "invalid.value");
        printProgress("Using: " + p);
        p.validate();
        String[] errorKeys = p.getErrorKeys();
        if (errorKeys == null || errorKeys.length == 0) {
            fail(p.toString());
        } else {
            printProgress("Success: expected and got: " + Arrays.asList(errorKeys));
        }
    }

}
