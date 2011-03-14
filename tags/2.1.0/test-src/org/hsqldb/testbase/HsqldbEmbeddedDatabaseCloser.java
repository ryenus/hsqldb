package org.hsqldb.testbase;

/**
 *
 * @author cboucher
 */
public class HsqldbEmbeddedDatabaseCloser implements ConnectionFactory.EventListener {

    private HsqldbEmbeddedDatabaseCloser() {}

    public static final ConnectionFactory.EventListener Instance = new HsqldbEmbeddedDatabaseCloser();

//    @Override
    public void closedRegisteredObjects(ConnectionFactory source) {
        org.hsqldb.DatabaseManager.closeDatabases(-1);
    }

}
